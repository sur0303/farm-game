import { FarmApp } from './app/App';

const root = document.querySelector<HTMLElement>('#app');
if (!root) throw new Error('app root is missing');

const app = new FarmApp(root);
void app.start();